package net.dgg.framework.tac.elasticsearch.core.page.cache;

public class HPageGroup {
	private int inputPageNo;
	private int startPageNo;
	private int endPageNo;

	public HPageGroup(int inputPageNo, int startPageNo, int endPageNo) {
		this.inputPageNo = inputPageNo;
		this.startPageNo = startPageNo;
		this.endPageNo = endPageNo;
	}

	public int getStartPageNo() {
		return startPageNo;
	}

	public void setStartPageNo(int startPageNo) {
		this.startPageNo = startPageNo;
	}

	public int getEndPageNo() {
		return endPageNo;
	}

	public void setEndPageNo(int endPageNo) {
		this.endPageNo = endPageNo;
	}

	public int getInputPageNo() {
		return inputPageNo;
	}

	public void setInputPageNo(int inputPageNo) {
		this.inputPageNo = inputPageNo;
	}

	public boolean onlyHasStartPageNoOfClosest() {
		return (startPageNo != inputPageNo && startPageNo > 0) && (endPageNo == inputPageNo || endPageNo <= 0);
	}

	public boolean onlyHasEndPageNoOfClosest() {
		return (inputPageNo != endPageNo && endPageNo > 0) && (startPageNo == inputPageNo || startPageNo <= 0);
	}
}